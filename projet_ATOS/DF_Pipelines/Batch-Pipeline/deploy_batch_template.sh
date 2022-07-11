cp pipeline_batch.py ..
cd ..
python3 -m pipeline_batch --runner DataflowRunner --project smartlive --staging_location gs://etienne_files/staging \
--temp_location gs://etienne_files/temp --template_location gs://etienne_files/templates_test/batch_template --region europe-west1
rm pipeline_batch.py