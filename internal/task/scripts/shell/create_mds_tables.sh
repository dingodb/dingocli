#!/usr/bin/env bash

# Usage: create_mds_tables MdsBinPath MdsClusterId

g_mds_client=$1
# cluster id is only used for dingo-store in order multiply mds cluster clould be created in the same store cluster
g_cluster_id=$2 

# Log function with timestamp
function log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Error handling function
function error_exit() {
    log "ERROR: $1"
    exit 1
}

# Usage: initCoorList MdsV2ConfDir
#function init_coor_list() {
#    # Check if COORDINATOR_ADDR is set
#    if [ -z "$COORDINATOR_ADDR" ]; then
#      error_exit "COORDINATOR_ADDR environment variable is not set"
#    fi
#
#    log "Initializing coordinator list at $g_mds_conf"
#    echo "$COORDINATOR_ADDR" > "$g_mds_conf" || error_exit "Failed to write to $g_mds_conf"
#    log "Coordinator list initialized successfully"
#}

function create_tables() {
    # Check if binary exists and is executable
    if [ ! -x "$g_mds_client" ]; then
      error_exit "Mds client binary not found or not executable: $g_mds_client"
    fi

    # create tables
    echo "Creating MDS tables..."
    $g_mds_client --cmd=CreateAllTable --coor_addr=list://$COORDINATOR_ADDR --cluster_id=$g_cluster_id
    local ret=$?
    if [ $ret -ne 0 ]; then
      error_exit "Failed to create MDS tables (return code: $ret)"
    fi
    log "MDS tables created successfully"
}

# Main execution
log "Starting MDS tables creation process"
#init_coor_list
create_tables
log "All operations completed successfully"

