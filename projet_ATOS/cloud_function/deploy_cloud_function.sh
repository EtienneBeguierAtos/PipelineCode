gcloud functions deploy pipeline_trigger --runtime python37 --trigger-topic my_topic3 --region=europe-west1 \
--set-env-vars machine_type=n1-standard-4