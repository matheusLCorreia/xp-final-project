kubectl create namespace xp-project;

kubectl delete -f postgres-configmap.yaml;
#kubectl delete -f psql-pv.yaml;
#kubectl delete -f psql-claim.yaml;
kubectl delete -f ps-deployment.yaml;
kubectl delete -f ps-service.yaml;

#echo "Deleted by manifest..."

kubectl apply -f postgres-configmap.yaml;
kubectl apply -f psql-pv.yaml;
kubectl apply -f psql-claim.yaml;
kubectl apply -f ps-deployment.yaml;
kubectl apply -f ps-service.yaml;

#echo "Finished..."
