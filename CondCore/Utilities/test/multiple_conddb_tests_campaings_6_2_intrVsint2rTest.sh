set -euo pipefail
export PYTHONNOUSERSITE="1"

#for PAYLOAD_SIZE in 1000 10000 100000 1000000 1000000 100000000; do
#    for PAYLOAD_NUMBER in 1 10; do
#        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
#        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_3_n${PAYLOAD_NUMBER}
#    done
#done


for PAYLOAD_SIZE in 1000 10000 100000; do
    for PAYLOAD_NUMBER in 1; do
        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --aux-dest-db oradev --auxdest-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_6_2_intrVsint2rTest_n${PAYLOAD_NUMBER}
    done
done

for PAYLOAD_SIZE in 100000000 1000000 100000 10000 1000 ; do
    for PAYLOAD_NUMBER in 10; do
        echo "Running payload test with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --aux-dest-db oradev --auxdest-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign conddb_copy_6_2_intrVsint2rTest_n${PAYLOAD_NUMBER}
    done
done
