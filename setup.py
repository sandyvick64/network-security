from setuptools import find_packages, setup
from typing import List

def get_requirements() -> List[str]:
    """This function will return list of requirements"""
    requirement_list:List[str] = []
    try:
        with open('requirements.txt', 'r') as file:
            lines = file.readlines()
            print("lines are:::", lines)
            for line in lines:
                requirement = line.strip()
                # ignore empty lines and -e.
                if requirement and requirement!= '-e .':
                    requirement_list.append(requirement)
        return requirement_list

    except FileExistsError as e:
        print("Requirements.txt file not found", e)

print(get_requirements())

setup(
    name= "NetworkSecurity",
    version = "0.0.1",
    author_email = "sandyvick64@gmail.com",
    packages= find_packages(),
    install_requires = get_requirements()
)
                    