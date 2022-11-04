from handler import handler

if __name__ == "__main__":
    from ast import literal_eval
    # AWS_PROFILE=smblog
    SAMPLE_DATA = "None"
    # Choose one of the SAMPLE_DATA below for local development
    # from sample_input.glue_job_success import SAMPLE_DATA
    # from sample_input.glue_job_failure import SAMPLE_DATA
    # from sample_input.lambda_success import SAMPLE_DATA
    # from sample_input.lambda_failure import SAMPLE_DATA
    # from sample_input.glue_crawler_success import SAMPLE_DATA
    from sample_input.glue_crawler_failure import SAMPLE_DATA

    event_package = literal_eval(SAMPLE_DATA)
    print(handler(event_package, {}))