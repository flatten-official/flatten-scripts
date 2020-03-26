# Base image
FROM python:3.6

# Update container dependencies
RUN apt-get update

# Set workdir and copy everything from current directory into workdir
WORKDIR /confirmed_cases
COPY . .

# install script dependenciess
RUN pip install -r requirements.txt
