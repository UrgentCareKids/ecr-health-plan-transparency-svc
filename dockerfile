FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY src/ .
# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt 

ENTRYPOINT ["python3"]
# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
#CMD [ "app.handler" ]