from pathlib import Path


WORKFLOW = Path(".github/workflows/deploy-mcp.yml").read_text()


def test_candidate_contract_is_verified_before_activation():
    candidate_check = WORKFLOW.index('echo "candidate_task_contract_verified=true"')
    install_adapter = WORKFLOW.index(
        '"${privilege[@]}" install -m 644 "$source_file" "$app_dir/mcp_server.py"'
    )
    install_contract = WORKFLOW.index(
        '"${privilege[@]}" install -m 644 "$contract_file" "$app_dir/task_contract.py"'
    )

    assert 'PYTHONPATH="$(dirname "$source_file")"' in WORKFLOW
    assert candidate_check < install_adapter
    assert candidate_check < install_contract


def test_failed_activation_restores_complete_prior_service():
    assert 'cp -a "$app_dir/mcp_server.py" "$rollback_dir/mcp_server.py"' in WORKFLOW
    assert (
        'cp -a "$app_dir/task_contract.py" "$rollback_dir/task_contract.py"'
        in WORKFLOW
    )
    assert 'cp -a "$unit" "$rollback_dir/bridgeghl-mcp.service"' in WORKFLOW
    assert (
        'install -m 644 "$rollback_dir/mcp_server.py" "$app_dir/mcp_server.py"'
        in WORKFLOW
    )
    assert (
        'install -m 644 "$rollback_dir/task_contract.py" "$app_dir/task_contract.py"'
        in WORKFLOW
    )
    assert (
        'install -m 644 "$rollback_dir/bridgeghl-mcp.service" "$unit"' in WORKFLOW
    )
    assert '"${privilege[@]}" systemctl restart bridgeghl-mcp.service' in WORKFLOW
    assert 'echo "deployment_rolled_back=true"' in WORKFLOW


def test_activation_commands_are_explicitly_fail_closed():
    assert (
        '"${privilege[@]}" install -m 644 "$source_file" '
        '"$app_dir/mcp_server.py" || exit 1'
    ) in WORKFLOW
    assert (
        '"${privilege[@]}" install -m 644 "$contract_file" '
        '"$app_dir/task_contract.py" || exit 1'
    ) in WORKFLOW
    assert '"${privilege[@]}" tee "$unit" >/dev/null <<\'UNIT\' || exit 1' in WORKFLOW
    assert '"${privilege[@]}" systemctl daemon-reload || exit 1' in WORKFLOW
    assert (
        '"${privilege[@]}" systemctl enable --now '
        'bridgeghl-mcp.service || exit 1'
    ) in WORKFLOW
    assert (
        '"${privilege[@]}" systemctl restart '
        'bridgeghl-mcp.service || exit 1'
    ) in WORKFLOW


def test_rollback_restores_prior_enabled_and_active_states():
    assert "was_enabled=false" in WORKFLOW
    assert "was_active=false" in WORKFLOW
    assert "systemctl is-enabled --quiet bridgeghl-mcp.service" in WORKFLOW
    assert "systemctl is-active --quiet bridgeghl-mcp.service" in WORKFLOW
    assert 'if [[ "$was_enabled" == true ]]; then' in WORKFLOW
    assert '"${privilege[@]}" systemctl enable bridgeghl-mcp.service' in WORKFLOW
    assert '"${privilege[@]}" systemctl disable bridgeghl-mcp.service' in WORKFLOW
    assert 'if [[ "$was_active" == true ]]; then' in WORKFLOW
    assert '"${privilege[@]}" systemctl restart bridgeghl-mcp.service' in WORKFLOW
    assert '"${privilege[@]}" systemctl stop bridgeghl-mcp.service' in WORKFLOW


def test_live_contract_checks_gate_activation_success():
    verifier = (
        '/opt/bridgeghl-mcp-py311/bin/python "$app_dir/task_contract.py"'
    )
    verifier_index = WORKFLOW.rindex(verifier)
    success_index = WORKFLOW.index("active_verified=true", verifier_index)

    assert 'http://127.0.0.1:8001/mcp >"$tools_file" && \\' in WORKFLOW
    assert 'http://127.0.0.1:8000/openapi.json >"$live_openapi" && \\' in WORKFLOW
    assert verifier_index < success_index
    assert '"; then\n                  rm -f "$tools_file"' in WORKFLOW


def test_rollback_success_marker_is_fail_closed():
    marker = 'echo "deployment_rolled_back=true"'
    marker_index = WORKFLOW.index(marker)
    rollback_guard = WORKFLOW.index('if [[ "$rollback_ok" == true ]]; then')
    failure_marker = WORKFLOW.index('echo "deployment_rollback_failed=true"')

    assert rollback_guard < marker_index < failure_marker
    assert 'systemctl is-active --quiet bridgeghl-mcp.service || rollback_ok=false' in WORKFLOW
    assert 'install -m 644 "$rollback_dir/mcp_server.py" "$app_dir/mcp_server.py" || rollback_ok=false' in WORKFLOW
    assert 'install -m 644 "$rollback_dir/task_contract.py" "$app_dir/task_contract.py" || rollback_ok=false' in WORKFLOW
