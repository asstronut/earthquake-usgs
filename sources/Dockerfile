FROM apache/beam_python3.10_sdk:2.52.0

# Prebuilt python dependencies
RUN pip install lxml
# Prebuilt other dependencies
RUN apt-get update \
  && apt-get dist-upgrade \
  && apt-get install -y --no-install-recommends ffmpeg

# Set the entrypoint to the Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]