"""Run after daily/KPL/limits/suspension commits; publish an atomic ladder generation."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
from limitup_ladder.build import main
if __name__=='__main__': main()
