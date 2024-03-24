from setuptools import setup, find_packages
packages_to_include = find_packages(exclude = ['test.*', 'test', 'test_manual'])
setup(
    name = 'santoshcloudaeonnetteam_projectgitsantosh',
    version = '0.8',
    packages = packages_to_include,
    description = '',
    install_requires = [
'Faker==24.3.0', 'pandas==2.2.1', ],
    data_files = ["resources/extensions.idx"]
)
