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
