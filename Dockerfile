FROM python:3.7-stretch AS build-env
WORKDIR /usr/src/app
COPY . .
# add here nexus pip conf 
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN echo "Build Completed."
# Building Distroless Container
FROM gcr.io/distroless/python3-debian10
COPY --from=build-env /usr/src/app /usr/src/app
COPY --from=build-env /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages
WORKDIR /usr/src/app
ENV PYTHONPATH=/usr/local/lib/python3.7/site-packages
CMD ["main.py"]