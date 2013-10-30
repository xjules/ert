/*
   Copyright (C) 2012  Statoil ASA, Norway. 
    
   The file 'enkf_plot_member.h' is part of ERT - Ensemble based Reservoir Tool. 
    
   ERT is free software: you can redistribute it and/or modify 
   it under the terms of the GNU General Public License as published by 
   the Free Software Foundation, either version 3 of the License, or 
   (at your option) any later version. 
    
   ERT is distributed in the hope that it will be useful, but WITHOUT ANY 
   WARRANTY; without even the implied warranty of MERCHANTABILITY or 
   FITNESS FOR A PARTICULAR PURPOSE.   
    
   See the GNU General Public License at <http://www.gnu.org/licenses/gpl.html> 
   for more details. 
*/


#ifndef __ENKF_PLOT_MEMBER_H__
#define __ENKF_PLOT_MEMBER_H__

#ifdef __cplusplus
extern "C" {
#endif
#include <time.h>
#include <stdbool.h>

#include <ert/util/util.h>

#include <ert/enkf/enkf_fs.h>
#include <ert/enkf/enkf_types.h>
#include <ert/enkf/enkf_node.h>
#include <ert/enkf/enkf_plot_arg.h>
  
  typedef struct enkf_plot_member_struct enkf_plot_member_type;
  
  UTIL_SAFE_CAST_HEADER( enkf_plot_member );
  UTIL_IS_INSTANCE_HEADER( enkf_plot_member );

  enkf_plot_member_type * enkf_plot_member_alloc( );
  void                    enkf_plot_member_load( enkf_plot_member_type * plot_member , enkf_node_type * enkf_node , enkf_fs_type * fs , const char * user_key , int iens , state_enum state , bool time_mode , int step1 , int step2);
  void                    enkf_plot_member_load__( void *arg );
  void                    enkf_plot_member_free__( void * arg );
  void                    enkf_plot_member_free( enkf_plot_member_type * plot_member );
  int                     enkf_plot_member_size( const enkf_plot_member_type * plot_member );
  void                    enkf_plot_member_iset( enkf_plot_member_type * plot_member , int index , time_t time , double value);
  double                  enkf_plot_member_iget_value( const enkf_plot_member_type * plot_member , int index);
  time_t                  enkf_plot_member_iget_time( const enkf_plot_member_type * plot_member , int index);
  bool                    enkf_plot_member_iget_active( const enkf_plot_member_type * plot_member , int index);
  bool                    enkf_plot_member_all_active( const enkf_plot_member_type * plot_member );

#ifdef __cplusplus
}
#endif
#endif
