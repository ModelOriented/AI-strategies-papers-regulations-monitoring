ARG docker_user
FROM ${docker_user}/common

COPY reporting_app .
EXPOSE 8080
ENTRYPOINT ["poetry", "run"]
CMD bash -c "streamlit run app.py --server.enableCORS false --server.enableXsrfProtection false --server.headless true --server.enableWebsocketCompression false"
