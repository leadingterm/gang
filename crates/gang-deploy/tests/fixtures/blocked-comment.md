<!-- gang-deploy-plan -->
## Infrastructure plan

Tree: `8888888888888888888888888888888888888888`

**Blocked: this revision must not merge.** The plan deletes or replaces protected resources:

- `aws_ebs_volume.data[0]` (aws_ebs_volume: delete)
- `aws_eip.gateway[0]` (aws_eip: delete)
- `aws_iam_outbound_web_identity_federation.main[0]` (aws_iam_outbound_web_identity_federation: delete)
- `aws_kms_key.state` (aws_kms_key: delete,create)
- `aws_s3_bucket.state[0]` (aws_s3_bucket: delete)

| Action | Count |
| --- | ---: |
| create | 1 |
| update | 0 |
| delete | 5 |
| replace | 1 |

Changed resources:

- `aws_ebs_volume.data[0]` (delete)
- `aws_eip.gateway[0]` (delete)
- `aws_iam_outbound_web_identity_federation.main[0]` (delete)
- `aws_instance.worker[0]` (delete)
- `aws_kms_key.signer` (create)
- `aws_kms_key.state` (delete, create)
- `aws_s3_bucket.state[0]` (delete)

