### Data Overview:
The `Data` directory presents the targeted repositories, the collected commits, and the filtered commits and subdomain takeover collection for the study.

We use SHA-256 (via Python’s hashlib.sha256()) to convert the original string to a cryptographically secure hash value due to some of data is sensitively related to subdomain that can be a takeover.

- **takeover_domain_final.7z**:  
  Lists the final vulnerable subdomain collection.
