"""Mix QC: the defects that are audible but invisible in a spectrogram."""
import sys, numpy as np, soundfile as sf
from scipy.signal import butter, sosfilt, hilbert

def qc(path):
    x,sr=sf.read(path); m=x.mean(axis=1)
    issues=[]
    # 1. Inter-sample clipping / squashed peaks
    clipped=int(np.sum(np.abs(m)>0.999))
    if clipped>50: issues.append(f"clipping: {clipped} samples at full scale")
    # 2. DC offset
    dc=float(np.mean(m))
    if abs(dc)>0.001: issues.append(f"DC offset {dc:+.4f}")
    # 3. Sidechain pump actually working? Low-band envelope should dip on kicks.
    sos=butter(4,[80,3000],'band',fs=sr,output='sos')
    env=np.abs(hilbert(sosfilt(sos,m)[::16]))
    er=sr/16
    kicks=[t for t in np.arange(18.0,26.0,0.5)]     # drop section, 4-on-floor
    dips=[]
    for k in kicks:
        i=int(k*er)
        pre=env[max(0,i-int(0.12*er)):i].mean()+1e-9
        at =env[i:i+int(0.05*er)].mean()+1e-9
        dips.append(20*np.log10(at/pre))
    pump=float(np.mean(dips))
    # 4. Kick/bass phase: correlation of sub band with the full mix at kicks
    sub=sosfilt(butter(4,120,'lp',fs=sr,output='sos'),m)
    sub_peak=[float(np.max(np.abs(sub[int(k*sr):int(k*sr)+int(0.1*sr)]))) for k in kicks]
    # 5. Stereo width (mid/side ratio)
    mid=(x[:,0]+x[:,1])/2; side=(x[:,0]-x[:,1])/2
    width=20*np.log10((np.sqrt(np.mean(side**2))+1e-12)/(np.sqrt(np.mean(mid**2))+1e-12))
    print(f"{path:18s} pump={pump:+5.1f}dB  width(S/M)={width:5.1f}dB  "
          f"sub@kick={np.mean(sub_peak):.2f}  clip={clipped:5d}  dc={dc:+.5f}")
    for i in issues: print(f"    !! {i}")

for a in sys.argv[1:]: qc(a)
