"""How far does each SFX poke above the bed it lands on?

This is what the ear calls "the effects are much louder than the music": not
the absolute level of either, but the transient's height over the surrounding
music floor.
"""
import sys, numpy as np, soundfile as sf
EVENTS=[('impact',2.80),('bank',6.10),('impact',8.20),('boost',10.35)]
for path in sys.argv[1:]:
    x,sr=sf.read(path); m=x.mean(axis=1)
    print(f"\n{path}")
    for name,t in EVENTS:
        # music floor: the 0.6 s BEFORE the hit, which is bed only
        pre=m[int((t-0.65)*sr):int((t-0.05)*sr)]
        floor=20*np.log10(np.sqrt(np.mean(pre**2))+1e-12)
        # the hit itself
        seg=m[int(t*sr):int((t+0.35)*sr)]
        pk=20*np.log10(np.max(np.abs(seg))+1e-12)
        rms=20*np.log10(np.sqrt(np.mean(seg**2))+1e-12)
        verdict='' if pk-floor < 9 else '   <<< STICKS OUT'
        print(f"  {name:7s}@{t:5.2f}  bed floor={floor:6.1f}  hit peak={pk:6.1f} "
              f"(+{pk-floor:4.1f})  hit rms={rms:6.1f} (+{rms-floor:4.1f}){verdict}")
