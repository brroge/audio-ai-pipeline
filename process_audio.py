import boto3
import time
import os
import urllib.request
import json

# Load environment variables
BUCKET_NAME = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION")
VOICE_ID = "Joanna"

# Initialize AWS clients with region
s3 = boto3.client("s3", region_name=AWS_REGION)
transcribe = boto3.client("transcribe", region_name=AWS_REGION)
polly = boto3.client("polly", region_name=AWS_REGION)

def upload_audio(file_path):
    filename = os.path.basename(file_path)
    s3.upload_file(file_path, BUCKET_NAME, f"audio_inputs/{filename}")
    print(f"✔ Uploaded: {filename}")
    return filename

def start_transcription(filename):
    # Ensure unique job name
    job_name = f"job_{int(time.time())}"
    file_uri = f"s3://{BUCKET_NAME}/audio_inputs/{filename}"

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={"MediaFileUri": file_uri},
        MediaFormat="mp3",
        LanguageCode="en-US"
    )

    print("🎤 Transcribing...")

    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        state = status["TranscriptionJob"]["TranscriptionJobStatus"]

        if state == "COMPLETED":
            print("✔ Transcription finished")
            break
        if state == "FAILED":
            print("❗ Transcription FAILED")
            return None

        time.sleep(3)

    transcript_url = status["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    return transcript_url

def download_transcript(transcript_url, filename):
    response = urllib.request.urlopen(transcript_url)
    data = json.loads(response.read())

    text = data["results"]["transcripts"][0]["transcript"]

    transcript_text_key = f"transcripts/{filename.replace('.mp3','.txt')}"
    s3.put_object(Body=text, Bucket=BUCKET_NAME, Key=transcript_text_key)

    print(f"✔ Transcript saved to: {transcript_text_key}")
    return text

def synthesize_audio(text, filename):
    response = polly.synthesize_speech(
        Text=text,
        OutputFormat="mp3",
        VoiceId=VOICE_ID
    )

    audio_stream = response["AudioStream"].read()
    output_file = f"audio_outputs/{filename.replace('.mp3','')}_EN.mp3"
    s3.put_object(Body=audio_stream, Bucket=BUCKET_NAME, Key=output_file)

    print(f"✔ Generated speech uploaded as: {output_file}")

def process_audio_file(file_path):
    filename = upload_audio(file_path)
    transcript_url = start_transcription(filename)
    transcript_text = download_transcript(transcript_url, filename)
    synthesize_audio(transcript_text, filename)

if __name__ == "__main__":
    for file in os.listdir("audio_inputs/"):
        if file.endswith(".mp3"):
            full_path = os.path.join("audio_inputs/", file)
            print(f"====== Processing {file} ======")
            process_audio_file(full_path)

    print("\n🎉 ALL AUDIO PROCESSED SUCCESSFULLY 🎉")
