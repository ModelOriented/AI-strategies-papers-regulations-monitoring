FROM piotrp/mair_common

COPY reporting_app .
COPY data/labels_hagendorffEthicsAIEthics2020.csv ./data/
EXPOSE 8080
ENTRYPOINT ["poetry", "run"]
CMD bash -c "streamlit run app.py --server.enableCORS false --server.enableXsrfProtection false --server.headless true --server.enableWebsocketCompression false"
