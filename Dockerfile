ARG BASE_IMAGE
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base as template_launcher
# Use the custom container image as the base image
FROM $BASE_IMAGE

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY . ${WORKDIR}/

RUN ls -R ${WORKDIR}

ENV PYTHONPATH ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline.py"

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]