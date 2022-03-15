import sys
from glob import glob

import h5py
import zarr
import xarray
import numpy as np
from datetime import datetime

sys.path.insert(0, '/glade/u/home/ksha/WORKSPACE/AIES/')
from namelist import * 

with h5py.File(save_dir+'BC_domain_info.hdf', 'r') as h5io:
    bc_lon = h5io['bc_lon'][...]
    bc_lat = h5io['bc_lat'][...]
    etopo_bc = h5io['etopo_bc'][...]
    land_mask_bc = h5io['land_mask_bc'][...]
    
land_select = np.logical_not(land_mask_bc)

land_lon = bc_lon[land_select]
land_lat = bc_lat[land_select]
etopo_bc_land = etopo_bc[land_select]

N_fcst = 54
freq = 3.0
FCSTs = np.arange(9, 240+freq, freq)[:N_fcst] # fcst lead as hours

cr = "This dataset is created for AIES, NCAR/UCAR only."
email = "Contact:<yingkaisha@gmail.com>"
var_unit = 'Input_elevation: bilinear interp from ETOPO1; unit [m]\nInput_GEFS_APCP: GEFS reforecast ensemble mean (averaged from 5 members); unit [mm/3hr]\nInput_GEFS_PWAT: see Input_GEFS_APCP; unit [mm/3hr]\nTarget_ERA_APCP: ERA5 reanalysis (analysis time has been converted to ini+lead; it matches GEFS); unit [mm/3hr]'
coord_unit = 'fcst_lead [hr]\nland_lon [degree longidute]\nland_lat [degree latitude]'

for year in range(2000, 2020):
    print('Processing {}'.format(year))

    ini_info = 'initialization_day [00 UTC days in {}]'.format(year)

    # APCP
    with h5py.File(REFCST_dir+'En_mean_APCP_{}.hdf'.format(year), 'r') as h5io:
        apcp_bc = h5io['bc_mean'][...]
    apcp_bc_land = apcp_bc[..., land_select]

    apcp_bc_land = apcp_bc_land[:, :54, :]

    # PWAT
    with h5py.File(REFCST_dir+'En_mean_PWAT_{}.hdf'.format(year), 'r') as h5io:
        pwat_bc = h5io['bc_mean'][...]
    pwat_bc_land = pwat_bc[..., land_select]
    pwat_bc_land = pwat_bc_land[:, :54, :]

    # ERA5
    with h5py.File(ERA_dir+'ERA5_GEFS-fcst_{}.hdf'.format(year), 'r') as h5io:
        era_bc = h5io['era_fcst'][:, :, bc_inds[0]:bc_inds[1], bc_inds[2]:bc_inds[3]]
    era_bc_land = era_bc[..., land_select]

    era_bc_land = era_bc_land[:, :54, :]

    # xarray dataset creation
    xDATA = xarray.Dataset(
        data_vars=dict(
            Input_elevation=(['BC_land_latlon',], etopo_bc_land),
            Input_land_lon=(['BC_land_latlon',], land_lon),
            Input_land_lat=(['BC_land_latlon',], land_lat),
            Input_GEFS_APCP=(['initialization_day', 'fcst_lead', 'BC_land_latlon'], apcp_bc_land),
            Input_GEFS_PWAT=(['initialization_day', 'fcst_lead', 'BC_land_latlon'], pwat_bc_land),
            Target_ERA_APCP=(['initialization_day', 'fcst_lead', 'BC_land_latlon'], era_bc_land),
        ),
        coords=dict(
            fcst_lead=FCSTs,
            lon=(['BC_land_latlon',], land_lon),
            lat=(['BC_land_latlon',], land_lat),
        ),
        attrs=dict(
            description=cr+'\n'+email+'\n'+var_unit+'\n'+coord_unit+'\n'+ini_info),
    )

    # Save
    xDATA.to_zarr('/glade/p/cisl/aiml/ksha/AI2ES/ysha_GEFS_ERA5_BC_{}.zarr'.format(year))

