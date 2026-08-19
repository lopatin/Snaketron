"""Is the sidechain actually pumping? Measure the recovery ramp BETWEEN kicks.

Measuring at the kick catches the kick's own energy, which reads as the
opposite of ducking. The audible signature of a pump is the level *rising*
through the gap between kicks.
"""
import sys, numpy as np, soundfile as sf
from scipy.signal import butter, sosfilt, hilbert
for path in sys.argv[1:]:
    x,sr=sf.read(path); m=x.mean(axis=1)
    # 300 Hz-4 kHz: above the kick's body, where the pads and arp live.
    band=sosfilt(butter(4,[300,4000],'band',fs=sr,output='sos'),m)
    env=np.abs(hilbert(band[::16])); er=sr/16
    ramps=[]
    for k in np.arange(18.0,26.0,0.5):
        a=int((k+0.02)*er); b=int((k+0.48)*er)
        early=env[a:a+int(0.08*er)].mean()+1e-12
        late =env[b-int(0.08*er):b].mean()+1e-12
        ramps.append(20*np.log10(late/early))
    print(f"{path:18s} recovery ramp between kicks: {np.mean(ramps):+5.1f} dB "
          f"({'pumping' if np.mean(ramps)>1.5 else 'FLAT - no audible pump'})")
