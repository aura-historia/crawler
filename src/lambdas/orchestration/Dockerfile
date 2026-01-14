FROM public.ecr.aws/lambda/python:3.11-x86_64

WORKDIR ${LAMBDA_TASK_ROOT}

COPY . .

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r src/lambdas/orchestration/requirements.txt -t .

ENV PYTHONPATH=${LAMBDA_TASK_ROOT}

# Lambda entrypoint
CMD ["src.lambdas.orchestration.orchestration_handler.handler"]

