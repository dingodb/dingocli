#!/usr/bin/env bash

# usage:
# curl -sSL https://raw.githubusercontent.com/dingodb/dingocli/main/scripts/install_dingo.sh | bash -s -- [--source=internal|github|local] [--branch=BRANCH] [local_binary_path_if_source_is_local]
# bash install_dingo.sh --source=internal|github|local [--branch=BRANCH] [local_binary_path_if_source_is_local]

############################  GLOBAL VARIABLES
g_color_yellow=$(printf '\033[33m')
g_color_red=$(printf '\033[31m')
g_color_normal=$(printf '\033[0m')
g_dingo_home="${HOME}/.dingo"
g_bin_dir="${g_dingo_home}/bin"
g_db_path="sqlite://${g_dingo_home}/data/dingocli.db"
g_profile="${HOME}/.profile"
g_internal_url="https://work.dingodb.top"
g_branch="${DINGO_BRANCH:-main}"
g_github_url="https://github.com/dingodb/dingocli/releases/download/${g_branch}/dingo"
g_upgrade="${DINGO_UPGRADE}"
g_version="${DINGO_VERSION:=$g_latest_version}"
g_download_url="${g_internal_url}/dingo.tar.gz"
g_local_binary=""

############################  BASIC FUNCTIONS
msg() {
    printf '%b' "${1}" >&2
}

success() {
    msg "${g_color_yellow}[✔]${g_color_normal} ${1}${2}"
}

die() {
    msg "${g_color_red}[✘]${g_color_normal} ${1}${2}"
    exit 1
}

program_must_exist() {
    local ret='0'
    command -v "${1}" >/dev/null 2>&1 || { local ret='1'; }

    if [ "${ret}" -ne 0 ]; then
        die "You must have '$1' installed to continue.\n"
    fi
}

############################ FUNCTIONS
backup() {
    if [ -d "${g_dingo_home}" ]; then
        mv "${g_dingo_home}" "${g_dingo_home}-$(date +%s).backup"
    fi
}

backup_binary() {
    if [ -f "${g_bin_dir}/dingo" ]; then
        mv "${g_bin_dir}/dingo" "${g_bin_dir}/dingo-$(date +%s).backup"
    fi
}

setup() {
    mkdir -p "${g_dingo_home}"/{bin,data,module,logs,temp}

    # generate config file
    local confpath="${g_dingo_home}/dingocli.cfg"
    if [ ! -f "${confpath}" ]; then
        cat << __EOF__ > "${confpath}"
[defaults]
log_level = error
sudo_alias = "sudo"
timeout = 300
auto_upgrade = false

[ssh_connections]
retries = 3
timeout = 10

[database]
url = "${g_db_path}"
#url = "rqlite://ip:port"
__EOF__
    fi
}

install_binary() {
    local ret=1
    
    local source=$1
    if [ "$source" == "internal" ]; then
        echo "Downloading from internal source..."
        local tempfile="/tmp/dingo"
        # Add your internal download logic here
        wget --no-check-certificate "${g_download_url}" -O "${tempfile}" # internal
        if [ $? -eq 0 ]; then
            mv ${tempfile} ${g_bin_dir}/
            ret=$?
        fi
    elif [ "$source" == "github" ]; then
        echo "Downloading from GitHub..."
        local tempfile="/tmp/dingo"
        # check /tmp/dingo exists, if exists, remove it
        if [ -f "${tempfile}" ]; then
            echo "remove existing tempfile ${tempfile}"
            sudo rm -f "${tempfile}"
        fi
        # Add your GitHub download logic here
        wget $g_github_url -O "${tempfile}" # github
        if [ $? -eq 0 ]; then
            cp "${tempfile}" "${g_bin_dir}/"
            ret=$?
        fi
    elif [ "$source" == "local" ]; then
        echo "Using local binary..."
        cp "$g_local_binary" "${g_bin_dir}/dingo"
        ret=$?
    else
        echo "Invalid source specified. Please choose 'internal' or 'github'."
        exit 1
    fi

    # rm  "${tempfile}"
    if [ ${ret} -eq 0 ]; then
        chmod 755 "${g_bin_dir}/dingo"
    else
        die "Download dingo failed\n"
    fi
}

set_profile() {
    shell=$(echo "$SHELL" | awk 'BEGIN {FS="/";} { print $NF }')
    if [ -f "${HOME}/.${shell}_profile" ]; then
        g_profile="${HOME}/.${shell}_profile"
    elif [ -f "${HOME}/.${shell}_login" ]; then
        g_profile="${HOME}/.${shell}_login"
    elif [ -f "${HOME}/.${shell}rc" ]; then
        g_profile="${HOME}/.${shell}rc"
    fi

    case :${PATH}: in
        *:${g_bin_dir}:*) ;;
        *) echo "export PATH=${g_bin_dir}:\${PATH}" >> "${g_profile}" ;;
    esac
}

print_install_success() {
    success "Install dingo ${g_version} success, please run 'source ${g_profile}'\n"
}

print_upgrade_success() {
    if [ -f "${g_dingo_home}/CHANGELOG" ]; then
        cat "${g_dingo_home}/CHANGELOG"
    fi
    success "Upgrade dingo to ${g_version} success\n"
}

install() {
    local source=$1
    backup
    setup
    install_binary "$source"
    set_profile
    print_install_success
}

upgrade() {
    local source=$1
    backup_binary
    install_binary "$source"
    print_upgrade_success
}

main() {
    local source="github"  # Default source
    # print all arguments
    for arg in "$@"; do
        case $arg in
            --source=*)
            source="${arg#*=}"
            if [ $source == "local" ]; then
                if [ -z "$2" ]; then
                    die "Please provide the local binary path when using --source=local\n"
                fi
                echo "Using local binary: $2"
                g_local_binary="$2"
                if [ ! -f "$g_local_binary" ]; then
                    die "Local binary file does not exist: $g_local_binary\n"
                fi
            fi
            shift
            ;;
            --branch=*)
            g_branch="${arg#*=}"
            # Update the GitHub URL with the new branch
            g_github_url="https://github.com/dingodb/dingocli/releases/download/${g_branch}/dingo"
            echo "Using branch: ${g_branch}"
            shift
            ;;
            *)
            # Unknown option
            ;;
        esac
    done
    if [ "${g_upgrade}" == "true" ]; then
        upgrade "$source"
    else
        install "$source"
    fi
}

############################  MAIN()
main "$@"
