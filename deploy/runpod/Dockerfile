# Service-only thin execution endpoint. Models remain ephemeral startup downloads.
FROM ghcr.io/lyj97/h3-comfy@sha256:1d9fddcced68175f6f0fd28371623a71bc9cad40cad8156e267e03c27062a22d
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server \
    && rm -rf /var/lib/apt/lists/* /etc/ssh/ssh_host_*
COPY deploy/runpod/start-ssh.sh /opt/h3-service/start-ssh.sh
COPY shared/*.py /opt/h3-service/shared/
COPY shared/execution_profiles/*.json /opt/h3-service/shared/execution_profiles/
COPY client/h3worker/*.py /opt/h3-service/client/h3worker/
COPY client/vace_smoke.py client/vace_reconcile.py /opt/h3-service/client/
COPY client/comfy_h3_contract_node /opt/h3-service/client/comfy_h3_contract_node
COPY client/comfy_h3_reference_node /opt/h3-service/client/comfy_h3_reference_node
COPY burst/h3burst /opt/h3-service/burst/h3burst
COPY deploy/runpod/check_image.py /opt/h3-service/check_image.py
COPY deploy/runpod/build_nodes.py /opt/h3-service/build_nodes.py
COPY deploy/runpod/build_audio.py /opt/h3-service/build_audio.py
COPY deploy/runpod/build_audio.py /opt/h3-service/deploy/runpod/build_audio.py
COPY deploy/runpod/check_audio.py /opt/h3-service/check_audio.py
RUN python3 -m pip install --no-cache-dir starlette==1.6.0 uvicorn==0.52.4 && python3 -m pip check
ENV PYTHONPATH=/opt/h3-service:/opt/h3-service/client:/opt/h3-service/burst \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 H3POD_ACCEPTANCE_ENABLED=0 \
    H3POD_BAKED_NODES_REQUIRED=1
# Fixed node source and dependency checks belong in CI, before GPU rental.
RUN python3 -B /opt/h3-service/build_nodes.py \
    && python3 -B -c 'import json; from shared.execution import profiles; nodes={n["name"]:n for p in profiles() for n in p["nodes"]["nodes"]}; open("/opt/h3-nodes.json","w").write(json.dumps({"schema_version":1,"nodes":list(nodes.values())}))' \
    && python3 -B -m h3burst.prepare --manifest /opt/h3-nodes.json \
       --comfy-root /opt/comfyui-baked --state /opt/h3-node-preparation
RUN set -e; python3 -B /opt/h3-service/build_audio.py \
    && for runtime in qwen_tts qwen_align cosyvoice3; do \
         /opt/h3-audio/$runtime/bin/python -B /opt/h3-service/check_audio.py $runtime; \
       done
EXPOSE 22
ENTRYPOINT ["/bin/sh", "/opt/h3-service/start-ssh.sh"]
CMD []
