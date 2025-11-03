Here is the script setup we use for the subdomain takeover detection in our Study:

1. **sha2repo.py**: This script collects the repository name for the collected commit with the sha number in the WoC server.

2. **sha2blob.py**: This script collects the blob id for the collected commit with the sha number in the WoC server.
   
3. **blob2content.py**: This script collects the commit content for the collected commit with the blob id in the WoC server.

4. **filter_io.py.py**: This script filters the CNAME with github.io in the commit content.
   
5. **dig_domain.py**: This script uses the dig command to verify the CNAME record with the collected subdomain.

6. **verify_domain.py**: This script requests the subdomain and github.io to further verify the condition for subdomain takeover.

7. **collect_bg.py**: This script use GitHub API to get the background information for the owner of subdomain collection repository
