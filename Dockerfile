FROM quay.io/astronomer/astro-runtime:3.3-7-python-3.12

# Install the Airflow dependencies with uv, straight from pyproject.toml. uv ships in the base
# image, and -c /etc/pip-constraints.txt is the constraint file Astro Runtime uses for its own
# installs, so nothing Astronomer pins gets moved underneath us. The ONBUILD triggers of the base
# image have already run at this point, which is why pyproject.toml is here without a COPY and
# why requirements.txt is an empty placeholder.
USER root
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r pyproject.toml -c /etc/pip-constraints.txt
USER astro

# Seed the Simple Auth Manager password file so that, if you turn the login screen on (see
# docker-compose.override.yml), the credentials are admin/admin instead of a random password.
# Ignored while the login screen is off, which is the default.
RUN echo '{"admin": "admin"}' > /usr/local/airflow/simple_auth_manager_passwords.json.generated
