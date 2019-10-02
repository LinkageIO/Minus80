# to deploy a function, log into GCP via the gcloud tools, switch to the right project
# and deploy the function (e.g. echo) using the following syntax
echo "Deploying $1"
gcloud functions deploy $1 \
 --source https://source.developers.google.com/projects/minus80/repos/github_linkageio_minus80/moveable-aliases/master/paths/cloud_functions  \
 --runtime python37 \
 --trigger-http
