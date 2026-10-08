# Task-specific startup image; reuse the tested service and dependency layers.
ARG SERVICE_IMAGE
FROM ${SERVICE_IMAGE}
ARG PROFILE_ID
ENV H3POD_PROFILE_ID=${PROFILE_ID}
LABEL io.h3.execution.profile=${PROFILE_ID}
