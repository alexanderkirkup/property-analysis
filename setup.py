from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='property-analysis',
    version='0.1',
    description='Python tools to analyse UK property data',
    long_description=readme,
    url='https://github.com/alexanderkirkup/property-analysis',
    author='Alexander Kirkup',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    include_package_data=True,
    install_requires=['pandas', 'aiohttp']
)