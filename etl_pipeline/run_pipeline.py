import os  
import sys  

# Add the project root to Python path  
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_pipeline.etl_flow import main

if __name__ == "__main__":
    main()