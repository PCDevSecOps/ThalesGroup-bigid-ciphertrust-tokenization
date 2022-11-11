FROM python:3.7

WORKDIR /opt/app

COPY . .

RUN pip install --no-cache-dir -r python-requirements.txt

EXPOSE 5000

CMD ["python3", "-m", "app", "--host", "0.0.0.0", "--port", "5000"]