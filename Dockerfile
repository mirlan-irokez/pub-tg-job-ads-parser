FROM python:3.10-slim

#Working dir
WORKDIR /app

#Requirements file
COPY requirements.txt .

# Install libraries 
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install gunicorn

# Copy the code 
COPY . .

# Env variable for Google auth
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/YOUTH_AUTH_CREDENTIALS

# Expose port
EXPOSE 8080

# Command to run python script
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app"]
