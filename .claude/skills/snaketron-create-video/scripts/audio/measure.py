import sys, numpy as np, soundfile as sf
from scipy.signal import welch
BANDS=[(20,60),(60,250),(250,1000),(1000,2500),(2500,6000),(6000,12000),(12000,20000)]
def report(path,label=None):
    raw,sr=sf.read(path)
    if raw.ndim>1:
        mono=raw.mean(axis=1)
        # Mono fold-down loss: anything over ~6 dB means the stereo trick used
        # to widen it is cancelling on a phone speaker.
        loss=20*np.log10((np.max(np.abs(raw))+1e-12)/(np.max(np.abs(mono))+1e-12))
        if loss>6: print(f"  !! {path}: {loss:.1f} dB lost when summed to mono")
        x=mono
    else:
        x=raw
    f,p=welch(x,sr,nperseg=8192); tot=np.trapezoid(p,f)+1e-20
    peak=np.max(np.abs(x)); rms=np.sqrt(np.mean(x**2))
    bands=[10*np.log10((np.trapezoid(p[(f>=lo)&(f<hi)],f[(f>=lo)&(f<hi)])+1e-20)/tot) for lo,hi in BANDS]
    print(f"{(label or path):16s} peak={20*np.log10(peak+1e-12):6.1f} rms={20*np.log10(rms+1e-12):6.1f} "
          f"crest={20*np.log10(peak/(rms+1e-12)):4.1f} | " + " ".join(f"{b:6.1f}" for b in bands))
print(f"{'file':16s} {'':22s} {'crest':>4s} | " + " ".join(f"{lo//1000 if lo>=1000 else lo}-{hi//1000 if hi>=1000 else hi}".rjust(6) for lo,hi in BANDS))
for a in sys.argv[1:]: report(a)
