from pathlib import Path
import sys
from PyInstaller.utils.hooks import copy_metadata

root = Path(SPECPATH).parent
sys.path.insert(0, str(root))
from kestrel_drop import __version__

a = Analysis([str(root / 'packaging/native_entry.py')], pathex=[str(root)],
             datas=copy_metadata('bleak'), hiddenimports=[],
             excludes=['tkinter', 'streamlit', 'altair', 'pandas'], noarchive=False)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, [], exclude_binaries=True, name='kestrel-drop',
          console=True, strip=False, upx=False)
bundle = COLLECT(exe, a.binaries, a.datas, strip=False, upx=False, name='kestrel-drop')
if sys.platform == 'darwin':
    app = BUNDLE(bundle, name='KestrelDrop.app', bundle_identifier='cn.pku.kestreldrop',
                 version=__version__, info_plist={
                     'CFBundleDisplayName': 'Kestrel DROP',
                     'NSBluetoothAlwaysUsageDescription': 'Connect to Kestrel DROP devices to download measurements and manage settings.',
                     'NSBluetoothPeripheralUsageDescription': 'Connect to Kestrel DROP devices to download measurements and manage settings.',
                 })
