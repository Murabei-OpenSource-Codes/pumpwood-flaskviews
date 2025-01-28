"""setup."""
import os
import setuptools

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setuptools.setup(
    name='pumpwood-flaskviews',
    version='{VERSION}',
    include_package_data=True,
    license='BSD-3-Clause License',
    description='Assist creation of flask views in Pumpwood format..',
    long_description=README,
    long_description_content_type="text/markdown",
    url='https://github.com/Murabei-OpenSource-Codes/pumpwood-flaskviews',
    author='Murabei Data Science',
    author_email='a.baceti@murabei.com',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    install_requires=[
        "python-slugify>=6.1.1",
        "pumpwood-communication>=0.71",
        "pandas>=1.0",
        "markupsafe==2.0.1",
        "SQLAlchemy-Utils==0.37.8",
        "SQLAlchemy==1.3.19",
        "GeoAlchemy2==0.9.3",
    ],
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
