import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib
import zipfile

def get_dependencies():
    """
    Downloads and installs external package dependencies.

    1. Ensure user account has write permission to the install path of this package.
    2. Run sframe.get_dependencies() to download and install them.
    3. Restart Python and import sframe again.

    By running the above function, you agree to the following licenses.

    * libstdc++: https://gcc.gnu.org/onlinedocs/libstdc++/manual/license.html
    * xz: http://git.tukaani.org/?p=xz.git;a=blob;f=COPYING
    """
    if sys.platform != 'win32':
        return
    print("""
By running this function, you agree to the following licenses.

* libstdc++: https://gcc.gnu.org/onlinedocs/libstdc++/manual/license.html
* xz: http://git.tukaani.org/?p=xz.git;a=blob;f=COPYING
    """)

    print('Downloading xz.')
    (xzarchive_file, xzheaders) = urllib.urlretrieve('http://tukaani.org/xz/xz-5.2.1-windows.zip')
    xzarchive_dir = tempfile.mkdtemp()
    print('Extracting xz.')
    xzarchive = zipfile.ZipFile(xzarchive_file)
    xzarchive.extractall(xzarchive_dir)
    xz = os.path.join(xzarchive_dir, 'bin_x86-64', 'xz.exe')

    print('Downloading gcc-libs.')
    (dllarchive_file, dllheaders) = urllib.urlretrieve('http://repo.msys2.org/mingw/x86_64/mingw-w64-x86_64-gcc-libs-5.1.0-1-any.pkg.tar.xz')
    dllarchive_dir = tempfile.mkdtemp()

    print('Extracting gcc-libs.')
    prev_cwd = os.getcwd()
    os.chdir(dllarchive_dir)
    subprocess.check_call([xz, '-d', dllarchive_file])
    dllarchive_tar = tarfile.open(os.path.splitext(dllarchive_file)[0])
    dllarchive_tar.extractall()

    print('Copying gcc-libs into the installation directory.')
    package_dir = os.path.dirname(__file__)
    cython_dir = os.path.join(package_dir, 'cython')
    shutil.copyfile( \
        os.path.join('mingw64', 'bin', 'libgcc_s_seh-1.dll'), \
        os.path.join(cython_dir, 'libgcc_s_seh-1.dll'))
    shutil.copyfile( \
        os.path.join('mingw64', 'bin', 'libstdc++-6.dll'), \
        os.path.join(cython_dir, 'libstdc++-6.dll'))
