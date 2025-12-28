FROM public.ecr.aws/lambda/python:3.11-x86_64

WORKDIR ${LAMBDA_TASK_ROOT}

COPY src/core/aws/lambdas/shop_registration/requirements.txt .

RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r requirements.txt -t .

COPY src/core/aws/lambdas/shop_registration src/core/aws/lambdas/shop_registration

# Copy other src code
COPY src/core/aws/database src/core/aws/database
COPY src/core/utils src/core/utils

ENV PYTHONPATH=${LAMBDA_TASK_ROOT}

# Lambda entrypoint
CMD ["src.core.aws.lambdas.shop_registration.shop_registration_handler.handler"]

