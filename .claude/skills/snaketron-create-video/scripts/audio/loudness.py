"""Perceived loudness around each SFX, K-weighted (ITU-R BS.1770).

Flat RMS underweights 1-6 kHz, which is where the ear is most sensitive and
where a bright musical effect lives. An effect can measure quieter than the bed
on RMS and still be plainly the loudest thing in the film.
"""
import sys, numpy as np, soundfile as sf
from scipy.signal import bilinear_zpk, zpk2sos, sosfilt

def k_weight(x, sr):
    # BS.1770 stage 1: high-shelf ~+4 dB above 1.5 kHz. Stage 2: 38 Hz highpass.
    f0, G, Q = 1681.97, 3.9998, 0.7071
    K = np.tan(np.pi*f0/sr); Vh = 10**(G/20); Vb = Vh**0.499
    a0 = 1+K/Q+K*K
    b = np.array([(Vh+Vb*K/Q+K*K)/a0, 2*(K*K-Vh)/a0, (Vh-Vb*K/Q+K*K)/a0])
    a = np.array([1, 2*(K*K-1)/a0, (1-K/Q+K*K)/a0])
    y = sosfilt(np.hstack([b,a]).reshape(1,6), x)
    f0, Q = 38.13, 0.5003
    K = np.tan(np.pi*f0/sr); a0 = 1+K/Q+K*K
    b = np.array([1, -2, 1]); a = np.array([1, 2*(K*K-1)/a0, (1-K/Q+K*K)/a0])
    return sosfilt(np.hstack([b,a]).reshape(1,6), y)

EVENTS=[('impact(kill)',2.80),('bank(goal)',6.10),('impact(kill)',8.20),('boost',10.35)]
for path in sys.argv[1:]:
    x,sr=sf.read(path); m=k_weight(x.mean(axis=1), sr)
    print(f"\n{path}")
    for name,t in EVENTS:
        pre=m[int((t-0.65)*sr):int((t-0.05)*sr)]
        floor=-0.691+10*np.log10(np.mean(pre**2)+1e-12)
        # 400 ms momentary window, as a loudness meter would use
        seg=m[int(t*sr):int((t+0.40)*sr)]
        lm=-0.691+10*np.log10(np.mean(seg**2)+1e-12)
        # and the full duration of the effect
        seg2=m[int(t*sr):int((t+1.3)*sr)]
        lf=-0.691+10*np.log10(np.mean(seg2**2)+1e-12)
        flag='  <<< LOUDEST' if lm-floor>4 else ''
        print(f"  {name:13s}@{t:5.2f}  bed={floor:6.1f} LUFS  momentary={lm:6.1f} "
              f"(+{lm-floor:4.1f})  over 1.3s={lf:6.1f} (+{lf-floor:4.1f}){flag}")
