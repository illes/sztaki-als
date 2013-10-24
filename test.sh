JAR=/Users/balint/Desktop/strato/sztaki_als/sztaki-als/target/ALS-0.1-SNAPSHOT.jar
IN='/Users/balint/Desktop/strato/sztaki_als/sztaki-als/test_med.csv'
OUT='/Users/balint/Desktop/strato/sztaki_als/sztaki-als/test.out'
K=10
ITER=10

./bin/pact-client.sh run --jarfile $JAR --arguments 1 file://$IN file://$OUT $K $ITER


echo "./bin/pact-client.sh run"
echo "--jarfile "$JAR
echo "--arguments 1 file://"$IN" file://"$OUT" "$K" "$ITER