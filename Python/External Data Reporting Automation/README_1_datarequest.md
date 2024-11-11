# Research Funding External Data Request 
Welcome to the Research Funding external data request repository! Wellcome's Research Funding  In this repo, you will find notebooks that complete RF data request received from external organisation.
The aim of this project is to automate the data request process whilst providing  text to explain how the data request is completed. Jupyter notebooks was chosen as the most appropriate method of achieving these two aims.


This project was made in collaboration between Research Funding and Portfolio Analysis. 
This repository contains....


#### Table of Contents

⚙️ [Initial setup](#%EF%B8%8F-initial-set-up)  
📖 [Using Jupyter Notebooks](#-using_jupyter-notebooks)  
🏃‍♂️ [Running RF data request notebooks](#-running_rf_data_request_notebooks)  
🆕 [New RF external data request](#-new-rf-external-data-request)  

# ⚙️ Initial set-up

## Installing Python and Virtual environments 

Python is programming language used to run the data request notebooks. Virtual environments(venv) are storage units used to hold different python packages (tools e.g. the matplotlib package is for making graphs) for specific projects. 

To run the data request notebooks, it is important to have python and virtual environments installed on your operating system (Windows/Mac)

### Instructions for installing Python 
- Instructions for installing Python on either Mac or Windows can be found on [Real python](https://realpython.com/installing-python/#how-to-install-python-on-macos) website. I recommend using the 'The full installer' method when installing python onto Windows and 'The official installer' method for Mac. Next step installing Visual Studio Code.

### Instructions for installing Virtual environments 
 - Instructions for installing Python on either Mac or Windows can be found on [Python Packaging Authority](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/). Find your computer terminal and enter code from [Python Packaging Authority](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/. Please refer to this page if  you get stuck, you may have install 'pip' first.  However a summary of the code needed can be found below.

#### For Mac
- install pip 
```
python3 -m pip install --user --upgrade pip 
python3 -m pip --version
```
- install venv
```bash
python3 -m pip install --user virtualenv
```

#### For Windows
- install pip 
```
py -m pip install --upgrade pip
py -m pip --version
```
- install venv
```bash
py -m pip install --user virtualenv
```
## Installing  Visual Studio Code

Visual Studio Code is an integrated development environment (IDE). Essentially, its a software that allows users to run code efficiently. Its also great for jupyter-notebooks (more on this later). 
There are


For generic instructions on how to set-up your local machine, please refer to the [Wiki page](https://github.com/wellcometrust/datascience/wiki). For installing, set up the repository:

```bash
git clone git@github.com:wellcometrust/datascience.git
```

And then from the folder, build a virtual environment. If you have a mac or linux or any machine with bash, you can just run the following command from the terminal:

```bash
make virtualenv
```



# 📖 Using Jupyter Notebooks

# 🏃‍♂️ Running RF data request notebooks
# 🆕 New RF external data request