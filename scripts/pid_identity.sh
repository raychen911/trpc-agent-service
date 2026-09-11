#!/usr/bin/env bash
# Shared PID identity verification for start.sh and stop.sh
# This file is sourced, not executed directly.

# Verify a PID matches the exact expected service instance
# Arguments: pid expected_module expected_subcommand expected_host expected_port
# Returns: 0 if exact match, 1 otherwise
_verify_pid_identity() {
    local pid="$1"
    local expected_module="$2"      # e.g., "trpc_service._cli"
    local expected_subcommand="$3"  # e.g., "worker", "gateway", "admin"
    local expected_host="$4"
    local expected_port="$5"

    if [[ ! -f "/proc/${pid}/cmdline" ]]; then
        return 1
    fi

    # Read cmdline as array of null-separated arguments
    local cmdline_args=()
    local arg
    while IFS= read -r -d '' arg; do
        cmdline_args+=("$arg")
    done < "/proc/${pid}/cmdline" 2>/dev/null

    # Need at least: python -m module subcommand --host X --port Y
    if [[ ${#cmdline_args[@]} -lt 8 ]]; then
        return 1
    fi

    local module_index=""
    local found_host=""
    local found_port=""
    local host_count=0
    local port_count=0
    local i=0

    while [[ $i -lt ${#cmdline_args[@]} ]]; do
        local arg="${cmdline_args[$i]}"

        if [[ "$arg" == "-m" ]] && [[ $((i + 2)) -lt ${#cmdline_args[@]} ]]; then
            if [[ -n "${module_index}" ]]; then
                return 1
            fi
            module_index="$i"
        fi

        if [[ "$arg" == "--host" ]] && [[ $((i+1)) -lt ${#cmdline_args[@]} ]]; then
            host_count=$((host_count + 1))
            found_host="${cmdline_args[$((i+1))]}"
        fi
        if [[ "$arg" == "--port" ]] && [[ $((i+1)) -lt ${#cmdline_args[@]} ]]; then
            port_count=$((port_count + 1))
            found_port="${cmdline_args[$((i+1))]}"
        fi

        i=$((i+1))
    done

    if [[ -z "${module_index}" ]]; then
        return 1
    fi

    if [[ "${cmdline_args[$((module_index + 1))]}" != "${expected_module}" ]]; then
        return 1
    fi

    if [[ "${cmdline_args[$((module_index + 2))]}" != "${expected_subcommand}" ]]; then
        return 1
    fi

    if [[ ${host_count} -ne 1 ]] || [[ "${found_host}" != "${expected_host}" ]]; then
        return 1
    fi

    if [[ ${port_count} -ne 1 ]] || [[ "${found_port}" != "${expected_port}" ]]; then
        return 1
    fi

    return 0
}
