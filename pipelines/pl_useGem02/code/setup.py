from setuptools import setup, find_packages
setup(
    name = 'pl_useGem02',
    version = '1.0',
    packages = find_packages(include = ('pl_usegem02*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'pandas==2.2.1', 'prophecy-libs==1.8.12'],
    entry_points = {
'console_scripts' : [
'main = pl_usegem02.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
