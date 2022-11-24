FROM public.ecr.aws/lambda/python:3.8
COPY . ./
RUN pip3 install -r requirements.txt
RUN yum install -y wget make tar gzip && wget https://www.rarlab.com/rar/rarlinux-x64-612.tar.gz && tar -zxvf rarlinux-x64-612.tar.gz && cd rar && make
CMD ["main.lambda_handler"]

