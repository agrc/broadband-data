# Broadband Data Skid

[![Push Events](https://github.com/agrc/broadband-data/actions/workflows/push.yml/badge.svg)](https://github.com/agrc/broadband-data/actions/workflows/push.yml)

Updates the data behind broadband.ugrc.utah.gov.

Contact: Rebecca Dilg

Source: The FCC's api at broadbandmap.fcc.gov.

Destination: Four feature services in UBC's AGOL org (will need to be transitioned to UDOT or our AGOL when UBC's AGOL goes to UDOT).

1. Hex Level 8 (the main broadband service layer shared in the SGID)
1. Hex Level 7
1. Hex Level 6
1. A summary hex layer that includes both level 8 hex geometries and a table that can be related to those with one record per provider per technology showing the best speed offered.

The general idea is that we take the BSL location IDs from the FCC data and aggregate them to the H3 hexes. Due to CostQuest licensing, we can't make the fabric public, so we need to aggregate. We then show the best service available in eac hex. The three hex level layers group the hexes by provider, technology, and speed and then dissolve the hexes together so that you can identify what speed and tech is available from what provider in any given area. It's not super precise. See https://gis.utah.gov/blog/2025-11-04-new-broadband-service-data/ for more info about the process.
