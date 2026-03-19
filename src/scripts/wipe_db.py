"""Wipe AuraDB and start from scratch."""

# Imports
from src.utils.ops import clear_all_nodes
from src.utils.connectors import AuraDB


# Wipe
driver = AuraDB().connect_sync()
clear_all_nodes(driver)
driver.close()
