FROM lemitrade.synology.me:8443/lemitrade/lemitrade-backend-libs:dev

WORKDIR /app

COPY . /app
RUN rm -f /app/source/configs.json

# EXPOSE ${CONSOLE_PORT}
EXPOSE 8888
CMD ["python", "./source/main.py"]