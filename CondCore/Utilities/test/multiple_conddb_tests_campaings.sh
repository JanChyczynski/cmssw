set -euo pipefail


#for PAYLOAD_SIZE in 1000 10000 100000 1000000 1000000 100000000; do
#    for PAYLOAD_NUMBER in 1 10; do
#        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
#        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_n${PAYLOAD_NUMBER}
#    done
#done


for PAYLOAD_SIZE in 1000000 100000000; do
    for PAYLOAD_NUMBER in 10; do
        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_2_n${PAYLOAD_NUMBER}
    done
done


for PAYLOAD_SIZE in 10000000; do
    for PAYLOAD_NUMBER in 1 4 16 32 64 128; do
        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_4_s${PAYLOAD_SIZE}_expTo128
    done
done

for PAYLOAD_SIZE in 100000; do
    for PAYLOAD_NUMBER in 1 32 128 512 2048; do
        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_5_s${PAYLOAD_SIZE}_expTo2048
    done
done
