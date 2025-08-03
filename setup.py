from setuptools import setup, find_packages

setup(
    name='cifa_cleaning',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas>=2.2.3',
        'numpy>=2.2.2',
        'rapidfuzz>=3.12.1',
    ],
    author='Jeremy Feagan',
    author_email='jeremy_feagan@comcast.com',
    description='A package for cleaning and matching supply chain data.',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)