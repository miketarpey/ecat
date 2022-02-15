from setuptools import setup, find_packages

try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import setup, Command

version = {}
with open("ecat/version.py") as fp:
    exec(fp.read(), version)

setup(
    name='ecat',
    version=version["__version__"],
    url='https://github.com/miketarpey/ecat',
    author='Mike Tarpey',
    author_email='miketarpey@gmx.net',
    license='MIT',
    packages=find_packages(),
    description='Baxter: Classroom - eCatalogue interface',
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.20.0",
        "openpyxl>=3.0.6",
        "xlsxwriter>=1.3.2",
        "cx_oracle",
        "psycopg2",
        "pypyodbc"],
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ])
