set -euo pipefail

#export PYTHONNOUSERSITE="1" 
export TEST_CONDDB_COMM_SCHEMA=cms_conditions_test

for PAYLOAD_SIZE in 100000000 1000000; do
    for PAYLOAD_NUMBER in 10 1; do
        CAMPAIGN_NAME="conddb_copy_3B_p5_s${PAYLOAD_SIZE}_n${PAYLOAD_NUMBER}"
        echo "Running payload test campaign ${CAMPAIGN_NAME} with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --dest-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}
        
        echo "Running read test campaign ${CAMPAIGN_NAME} "
        ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}  --delete-dest-sqlite

    done
done


for PAYLOAD_SIZE in 10000000; do
    for PAYLOAD_NUMBER in 1 4 16 32 64 128; do
        CAMPAIGN_NAME="conddb_copy_4B_p5_s${PAYLOAD_SIZE}_n${PAYLOAD_NUMBER}"
        echo "Running payload test campaign ${CAMPAIGN_NAME} with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --dest-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}

        echo "Running read test campaign ${CAMPAIGN_NAME} "
        ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}  --delete-dest-sqlite
    done
done

for PAYLOAD_SIZE in 100000; do
    for PAYLOAD_NUMBER in 1 32 128 512 2048; do
        CAMPAIGN_NAME="conddb_copy_5B_p5_expTo2048_vocms_s${PAYLOAD_SIZE}_n${PAYLOAD_NUMBER}"
        echo "Running payload test campaign ${CAMPAIGN_NAME} with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --dest-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}

        echo "Running read test campaign ${CAMPAIGN_NAME} "
        ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}  --delete-dest-sqlite
    done
done


for PAYLOAD_SIZE in 180000000; do
    for PAYLOAD_NUMBER in 1 10; do
        CAMPAIGN_NAME="conddb_copy_3D_BIG_p5_s${PAYLOAD_SIZE}_n${PAYLOAD_NUMBER}"
        echo "Running payload test campaign ${CAMPAIGN_NAME} with size ${PAYLOAD_SIZE} and number ${PAYLOAD_NUMBER}"
        ./test_query_logging.sh --create-payloads --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --dest-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb --dest-schema cms_conditions_test --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}

        echo "Running read test campaign ${CAMPAIGN_NAME} "
        ./test_query_logging.sh --payload-size ${PAYLOAD_SIZE} --payload-number ${PAYLOAD_NUMBER} --executions 10 --source-db oracle://CMS_CONDITIONS_TEST@cmsintr_lb  --cmssw-path ${CMSSW_BASE}/src --campaign ${CAMPAIGN_NAME}  --delete-dest-sqlite

    done
done

