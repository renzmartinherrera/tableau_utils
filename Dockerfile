FROM python:3.9.7

WORKDIR /usr/src/tableau_online_usage

RUN /usr/local/bin/python -m pip install --upgrade pip

COPY . ./

RUN pip install --no-cache-dir -r snowflake_connector_requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Cron
RUN apt-get update -qq && apt-get install -y cron
COPY cronschedule /etc/cron.d/cronschedule
RUN chmod 0644 /etc/cron.d/cronschedule &&\
    crontab /etc/cron.d/cronschedule

# Install VIM
RUN ["apt-get", "install", "-y", "vim"] 

# Run a bash script on startup so the Python scripts have the environmental
# variables defined in Dockerfile
# https://stackoverflow.com/questions/27771781/how-can-i-access-docker-set-environment-variables-from-a-cron-job

# Start cron in the foreground so that the container stays running
CMD ["cron",  "-f"]
