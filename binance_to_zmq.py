"""
binance_to_zmq.py
Streams BTC/USDT trades from Binance → normalization → ZMQ PUSH → VCV Rack.
Packet: 57 float32 = 228 bytes (zmq_bridge format)

CV Output Map for VCV Rack:
- CV1 (cv[0]): Price drift from start (range ±10V)
- CV2 (cv[1]): Instantaneous rate of change (for pad - filter modulation input)
- CV3 (cv[2]): Gate "price goes up" (for "beep-sine osc" trigger)
- CV4 (cv[3]): Gate "price goes down"
- CV5 (cv[4]): Trade volume (for trade value filter)
- CV6 (cv[5]): Trigger dnbOn (10.0V if price > x, else 0.0V)
- CV7 (cv[6]): Trigger leadOn (10.0V if price > y, else 0.0V)
- CV8 (cv[7]): BPM Modulator CV (for Clock BPM modulation)
- POLY PITCH & POLY GATE: Lead melody control within Ab Major (F minor) scale
"""

import asyncio
import struct
import json
import math
import zmq
import websockets

# ── Settings ──────────────────────────────────────────────────────────
ZMQ_ADDR    = "tcp://127.0.0.1:5555"
WS_URL      = "wss://stream.binance.com:9443/ws/btcusdt@trade"
PRICE_RANGE = 5000.0   # ±$5000 → ±10V for price drift (CV1 & drift mode)

# Threshold Settings
# If USE_RELATIVE_THRESHOLDS = True, thresholds x and y are calculated relative to start price.
USE_RELATIVE_THRESHOLDS = True
X_OFFSET = 10.0   # dnbOn triggers on price increase +$10.0 from start
Y_OFFSET = 25.0   # leadOn triggers on price increase +$25.0 from start

# If USE_RELATIVE_THRESHOLDS = False, absolute price values are used:
X_ABSOLUTE = 67500.0
Y_ABSOLUTE = 67600.0

# Volume (quantity) mapping settings to CV (0-10V)
VOL_MIN = 0.001  # Minimum BTC quantity to map (gives 0.0V)
VOL_MAX = 1.0    # Maximum BTC quantity to map (gives 10.0V)

# BPM Modulation Settings (for CV8 / cv[7])
BPM_BASE        = 120.0  # Baseline BPM when price is at start_price (0V in direct_10bpm mode)
BPM_MIN         = 90.0   # Minimum allowed BPM (0V in linear_10v mode)
BPM_MAX         = 120.0  # Maximum allowed BPM (10V in linear_10v mode)
BPM_SENSITIVITY = 0.8    # BPM change per $1 of price change from start. 
                         # E.g., +$10.0 drift = +8.0 BPM (128 BPM total).
                         # -$30.0 drift = -24.0 BPM (96 BPM total).

# BPM CV Output Mode:
# - "direct_10bpm": 1V = 10 BPM change (0V = BPM_BASE, e.g. +0.8V = +8 BPM for 128 BPM). Ideal for Impromptu Clocked.
# - "linear_10v": 0V = BPM_MIN, 10V = BPM_MAX. Linear mapping across the entire range.
BPM_CV_MODE     = "direct_10bpm"

# Number of active polyphonic channels/voices (1-16)
# Set to 2 for alternating "guitar solo" effect between 2 instruments
NUM_VOICES = 2

# Melody Generation Settings
# Modes:
# - "walk": (Recommended) Price changes move the pitch step-by-step up/down the scale (melodic Brownian walk).
# - "ema_drift": Pitch maps to price drift relative to an Exponential Moving Average (auto-centering register).
# - "drift": (Original) Pitch maps directly to absolute price drift from start price.
MELODY_MODE = "walk"

# Sensitivity & parameters for melody modes:
MELODY_SENSITIVITY_WALK = 0.25  # Scale steps to move per $1 of price change (for "walk" mode)
MELODY_RANGE_EMA        = 100.0  # Range in dollars for ±2V drift (for "ema_drift" mode)
MELODY_EMA_ALPHA        = 0.02   # Smoothing factor for EMA (0.01 - 0.1, lower is slower/wider register)

# Lead guitar pitch range (in V/Oct relative to C4, where 0.0V = C4)
PITCH_MIN = 0.0   # C4 (middle C — bottom of the 2-octave solo range)
PITCH_MAX = 2.0   # C6 (top of the 2-octave solo range)

# Portamento / Glide / Lag coefficient (0.01 - 1.0)
# 1.0 = instant note changes (no lag/glide)
# Lower values (e.g., 0.15) add a smooth slide between notes (portamento/lag)
MELODY_GLIDE = 0.15

# Trigger mode settings
# If True, gates behave as short trigger pulses (10V for a short time, then 0V).
# If False, gates stay high until the next voice triggers.
GATE_AS_TRIGGER = True
TRIGGER_DURATION = 0.02  # Trigger pulse duration in seconds (e.g., 0.02 = 20ms)

# Note rate limiter
# Minimum time (in seconds) between consecutive note triggers.
# All price changes that arrive in this window are accumulated and used
# to pick a single next note — preventing Binance data bursts from
# firing 10 notes at once.  Think of it as the "tempo gate" for the solo.
# e.g.: 0.25 = max 4 notes/sec  |  0.5 = max 2 notes/sec
NOTE_INTERVAL = 0.35  # seconds between notes

# Scale: Ab Major / F natural minor
# Steps relative to C note (in Volts per Octave V/Oct):
# C=0.0, Db=1/12, Eb=3/12, F=5/12, G=7/12, Ab=8/12, Bb=10/12
AB_MAJOR_SCALE = [0.0, 1.0/12.0, 3.0/12.0, 5.0/12.0, 7.0/12.0, 8.0/12.0, 10.0/12.0]

# Generate a list of quantized V/Oct values for Ab Major scale within lead range
SCALE_NOTES = []
for oct_val in range(-3, 4):  # Check octaves from -3V to +3V
    for step in AB_MAJOR_SCALE:
        note_v = oct_val + step
        if PITCH_MIN <= note_v <= PITCH_MAX:
            SCALE_NOTES.append(note_v)


def build_packet(cv, pitch, gate, vel):
    """Упаковка в ZmqAudioFrame (228 байт = 57 float32)."""
    cv9     = (cv    + [0.0] * 9)[:9]
    pitch16 = (pitch + [0.0] * 16)[:16]
    gate16  = (gate  + [0.0] * 16)[:16]
    vel16   = (vel   + [0.0] * 16)[:16]
    return struct.pack('<57f', *cv9, *pitch16, *gate16, *vel16)


def price_to_voct(delta):
    """Дельта цены → V/Oct (±2V = ±2 октавы от C4)."""
    return max(-2.0, min(2.0, (delta / PRICE_RANGE) * 2.0))


def delta_to_cv(delta):
    """Дельта цены → диапазон ±10V."""
    return max(-10.0, min(10.0, (delta / PRICE_RANGE) * 10.0))


def quantize(v):
    """Quantize V/Oct to Ab Major scale (for harmonious lead melody)."""
    octave = int(v) if v >= 0 else int(v) - 1
    frac   = v - octave
    nearest = min(AB_MAJOR_SCALE, key=lambda x: abs(x - frac))
    return octave + nearest


def volume_to_cv(quantity):
    """Map trade volume (quantity in BTC) to 0-10V using logarithmic scaling."""
    if quantity <= VOL_MIN:
        return 0.0
    if quantity >= VOL_MAX:
        return 10.0
    return 10.0 * (math.log10(quantity) - math.log10(VOL_MIN)) / (math.log10(VOL_MAX) - math.log10(VOL_MIN))


def calculate_bpm_cv(delta):
    """Calculate the target BPM and its corresponding CV based on price drift."""
    bpm = BPM_BASE + delta * BPM_SENSITIVITY
    bpm = max(BPM_MIN, min(BPM_MAX, bpm))
    
    if BPM_CV_MODE == "direct_10bpm":
        # 1V = 10 BPM relative to BPM_BASE. E.g., at 128 BPM, CV is +0.8V.
        cv_val = (bpm - BPM_BASE) / 10.0
        return max(-10.0, min(10.0, cv_val)), bpm
    else:  # "linear_10v"
        # 0V = BPM_MIN, 10V = BPM_MAX
        cv_val = 10.0 * (bpm - BPM_MIN) / (BPM_MAX - BPM_MIN)
        return max(0.0, min(10.0, cv_val)), bpm


async def stream_binance(sock):
    start_price = None
    last_price  = None
    voice       = 0

    # Melody states
    scale_index_float = float(len(SCALE_NOTES) // 2)
    price_ema       = None
    last_note_time  = 0.0       # asyncio time of the last fired note
    price_delta_accum = 0.0     # accumulated price change since last note

    # Buffers for polyphonic voice (pitch retention and targets)
    target_pitch = [0.0] * 16
    midi_pitch   = [0.0] * 16
    midi_gate    = [0.0] * 16
    midi_vel     = [0.0] * 16

    print(f"[ZMQ] Connecting to {ZMQ_ADDR}...")
    print(f"[WS]  Connecting to Binance trade stream (BTC/USDT)...")

    async with websockets.connect(WS_URL) as ws:
        async for raw in ws:
            data     = json.loads(raw)
            price    = float(data["p"])   # Current trade price
            quantity = float(data["q"])   # Trade volume (quantity)

            if start_price is None:
                start_price = last_price = price
                
                # Calculate thresholds
                global x_thresh, y_thresh
                if USE_RELATIVE_THRESHOLDS:
                    x_thresh = start_price + X_OFFSET
                    y_thresh = start_price + Y_OFFSET
                else:
                    x_thresh = X_ABSOLUTE
                    y_thresh = Y_ABSOLUTE
                    
                print(f"[START] Start price: ${price:,.2f}")
                print(f"[CONFIG] dnbOn threshold (x): ${x_thresh:,.2f}")
                print(f"[CONFIG] leadOn threshold (y): ${y_thresh:,.2f}")
                continue

            d_start = price - start_price   # Дрейф относительно цены запуска
            d_last  = price - last_price    # Локальная скорость изменения цены

            # Рассчитываем состояния триггеров dnbOn и leadOn
            dnb_on = 10.0 if price > x_thresh else 0.0
            lead_on = 10.0 if price > y_thresh else 0.0

            # Calculate BPM and BPM CV
            bpm_cv, current_bpm = calculate_bpm_cv(d_start)

            # ── CV (Mono channels 1-8) ──────────────────────────────────
            cv = [
                delta_to_cv(d_start),           # CV1: Price drift
                delta_to_cv(d_last * 50),       # CV2: Rate of change (x50 multiplier)
                10.0 if d_last > 0 else 0.0,    # CV3: Gate "price goes up"
                10.0 if d_last < 0 else 0.0,    # CV4: Gate "price goes down"
                volume_to_cv(quantity),         # CV5: Trade volume (for filter)
                dnb_on,                         # CV6: dnbOn trigger
                lead_on,                        # CV7: leadOn trigger
                bpm_cv,                         # CV8: BPM Modulator CV
            ]

            # ── Polyphony: note rate limiter + pitch selection ───────────
            # Always accumulate the raw price delta (direction info).
            price_delta_accum += d_last

            now = asyncio.get_event_loop().time()
            if now - last_note_time >= NOTE_INTERVAL:
                last_note_time = now

                # Calculate pitch based on chosen MELODY_MODE using accumulated delta
                if MELODY_MODE == "walk":
                    scale_index_float += price_delta_accum * MELODY_SENSITIVITY_WALK
                    scale_index_float = max(0.0, min(len(SCALE_NOTES) - 1, scale_index_float))
                    pitch_v = SCALE_NOTES[int(round(scale_index_float))]

                elif MELODY_MODE == "ema_drift":
                    if price_ema is None:
                        price_ema = price
                    else:
                        price_ema = MELODY_EMA_ALPHA * price + (1.0 - MELODY_EMA_ALPHA) * price_ema
                    d_ema = price - price_ema
                    voct  = max(-2.0, min(2.0, (d_ema / MELODY_RANGE_EMA) * 2.0))
                    pitch_v = max(PITCH_MIN, min(PITCH_MAX, quantize(voct)))

                else:  # "drift"
                    pitch_v = max(PITCH_MIN, min(PITCH_MAX, quantize(price_to_voct(d_start))))

                # Reset accumulator after the note fires
                price_delta_accum = 0.0

                vel_v = volume_to_cv(quantity)

                # Turn off the previous voice if not in trigger mode
                if not GATE_AS_TRIGGER:
                    prev = (voice - 1) % NUM_VOICES
                    midi_gate[prev] = 0.0

                # Set new target pitch and fire gate
                target_pitch[voice] = pitch_v
                midi_gate[voice]    = 10.0
                midi_vel[voice]     = vel_v

                # Apply glide (portamento/lag) to all voices
                for i in range(16):
                    midi_pitch[i] = MELODY_GLIDE * target_pitch[i] + (1.0 - MELODY_GLIDE) * midi_pitch[i]

                # ── Send to ZMQ PUSH ──────────────────────────────────────
                sock.send(build_packet(cv, midi_pitch, midi_gate, midi_vel), zmq.NOBLOCK)

                # Schedule gate-off pulse
                if GATE_AS_TRIGGER:
                    async def trigger_off(v_idx):
                        await asyncio.sleep(TRIGGER_DURATION)
                        midi_gate[v_idx] = 0.0
                        sock.send(build_packet(cv, midi_pitch, midi_gate, midi_vel), zmq.NOBLOCK)
                    asyncio.create_task(trigger_off(voice))

                voice = (voice + 1) % NUM_VOICES

            else:
                # Not time for a new note yet — still send CV updates (filter, drift, etc.)
                sock.send(build_packet(cv, midi_pitch, midi_gate, midi_vel), zmq.NOBLOCK)

            last_price = price
            
            # Console logging
            direction = "▲ UP  " if d_last > 0 else "▼ DOWN" if d_last < 0 else "  STAY"
            dnb_status = "dnbON" if dnb_on > 0 else "dnbOFF"
            lead_status = "leadON" if lead_on > 0 else "leadOFF"
            print(f"[{direction}] Price: ${price:>10,.2f} | "
                  f"Drift: {d_start:+.2f}$ | "
                  f"Vol: {quantity:4.3f} | "
                  f"BPM: {current_bpm:.1f} ({bpm_cv:+.2f}V) | "
                  f"[{dnb_status}] [{lead_status}] | "
                  f"Pitch: {pitch_v:+.3f}V")


def main():
    ctx  = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    
    # Режим PUSH. Подключаемся к VCV Rack (который слушает как PULL)
    sock.connect(ZMQ_ADDR)
    try:
        asyncio.run(stream_binance(sock))
    except KeyboardInterrupt:
        print("\n[STOP] Script stopped by user.")
    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    main()
