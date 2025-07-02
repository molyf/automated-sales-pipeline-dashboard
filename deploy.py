from etl_pipeline.etl_flow import main 

if __name__ == "__main__":
    main.deploy(
        name="etl-deployment",
        work_pool_name="etl-workpool",  # Replace this
    )
