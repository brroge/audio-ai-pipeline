import boto3
import time
import os

import json

s3_bucket = os.getenv("S3_BUCKET")
target_lang = os.getenv("TARGET_LANGUAGE", "es")  # default Spanish

transcribe = boto3.client("transcribe")
translate = boto3.client("translate")
polly = boto3.client("polly")
s3 = boto3.client("s3")

def upload_audio(file_path, file_name):
    print(f"Uploading {file_name} to S3...")
    s3.upload_file(file_path, s3_bucket, f"audio_inputs/{file_name}")

def start_transcription_job(file_name):
    job_name = f"transcribe_{file_name.replace('.mp3','')}"
    media_uri = f"s3://{s3_bucket}/audio_inputs/{file_name}"

    print("Starting Transcribe job...")

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={"MediaFileUri": media_uri},
        MediaFormat="mp3",
        LanguageCode="en-US"
    )

    # Wait until the job finishes
    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        state = status["TranscriptionJob"]["TranscriptionJobStatus"]

        if state in ["COMPLETED", "FAILED"]:
            break
        print("Transcribing...")
        time.sleep(5)

    print("Transcription complete.")

    # Download transcript URL
    transcript_url = status["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
    text_file = f"{file_name.replace('.mp3','')}.txt"

    # Download transcript JSON
    transcript_json = boto3.client("s3").get_object(
        Bucket=s3_bucket,
        Key=f"transcripts/{text_file}"
    )

def process_transcript(js):
    return js["results"]["transcripts"][0]["transcript"]

def translate_text(text):
    print("Translating text...")
    response = translate.translate_text(
        Text=text,
        SourceLanguageCode="en",
        TargetLanguageCode=target_lang
    )
    return response["TranslatedText"]

def synthesize_speech(text, file_name):
    print("Generating speech via Polly...")
    response = polly.synthesize_speech(
        Text=text,
        OutputFormat="mp3",
        VoiceId="Joanna"
    )
    output_file = f"{file_name.replace('.mp3','')}_{target_lang}.mp3"

    with open(output_file, "wb") as f:
        f.write(response["AudioStream"].read())

    return output_file


def main():
    for file_name in os.listdir("audio_inputs"):
        if file_name.endswith(".mp3"):
            file_path = os.path.join("audio_inputs", file_name)

            print(f"Processing {file_name}...")

            # Upload audio
            upload_audio(file_path, file_name)

            # Run Transcribe
            start_transcription_job(file_name)

            # Download transcript JSON file from S3
            transcript_key = f"transcripts/{file_name.replace('.mp3','')}.json"
            transcript_obj = s3.get_object(Bucket=s3_bucket, Key=transcript_key)
            transcript_json = json.loads(transcript_obj["Body"].read())

            # Extract transcript text
            transcript_text = transcript_json["results"]["transcripts"][0]["transcript"]

            # Save transcript to S3
            s3.put_object(
                Bucket=s3_bucket,
                Key=f"transcripts/{file_name.replace('.mp3','')}.txt",
                Body=transcript_text
            )

            # Translate
            translated = translate_text(transcript_text)

            # Save translation
            translation_key = f"translations/{file_name.replace('.mp3','')}_{target_lang}.txt"
            s3.put_object(
                Bucket=s3_bucket,
                Key=translation_key,
                Body=translated
            )

            # Generate audio (Polly)
            output_audio = synthesize_speech(translated, file_name)

            # Upload synthesized audio
            s3.upload_file(
                output_audio,
                s3_bucket,
                f"audio_outputs/{output_audio}"
            )

            print("DONE!")


if __name__ == "__main__":
    main()
