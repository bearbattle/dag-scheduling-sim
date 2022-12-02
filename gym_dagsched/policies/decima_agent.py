from time import time

import torch
import torch.nn as nn
from torch_geometric.nn import MessagePassing
from torch_geometric.utils import add_self_loops, degree
import torch_geometric.nn as gnn
from torch_scatter import segment_add_csr

from gym_dagsched.utils.device import device


def make_mlp(in_ch, out_ch, h1=32, h2=16, h3=8):
    return nn.Sequential(
        nn.Linear(in_ch, h1),   nn.ReLU(),
        nn.Linear(h1, h2),      nn.ReLU(),
        nn.Linear(h2, h3),      nn.ReLU(),
        nn.Linear(h3, out_ch)
    )
        


class GCNConv(MessagePassing):
    def __init__(self, in_ch, out_ch):
        super().__init__(aggr='add', flow='target_to_source')
        self.mlp1 = make_mlp(in_ch, 8)
        self.mlp2 = make_mlp(8, out_ch)
        

    def forward(self, x, edge_index):
        edge_index, _ = add_self_loops(edge_index, num_nodes=x.size(0))

        x = self.mlp1(x)

        row, col = edge_index
        deg = degree(col, x.size(0), dtype=x.dtype)
        deg_inv_sqrt = deg.pow(-0.5)
        deg_inv_sqrt[deg_inv_sqrt == float('inf')] = 0
        norm = deg_inv_sqrt[row] * deg_inv_sqrt[col]

        return self.propagate(edge_index, x=x, norm=norm)


    def message(self, x_j, norm):
        return norm.view(-1, 1) * x_j

    
    def update(self, aggr_out):
        x = self.mlp2(aggr_out)
        return x
    
    

class GraphEncoderNetwork(nn.Module):
    def __init__(self, num_node_features, dim_embed):
        super().__init__()
        self.conv1 = GCNConv(num_node_features, dim_embed)
        self.mlp_dag = make_mlp(num_node_features + dim_embed, dim_embed)
        self.mlp_global = make_mlp(dim_embed, dim_embed)


    def forward(self, dag_batch, env_indptr):
        x = self._compute_node_level_embeddings(dag_batch)
        y = self._compute_dag_level_embeddings(x, dag_batch)
        z = self._compute_global_embeddings(y, env_indptr)
        return x, y, z

    
    def _compute_node_level_embeddings(self, dag_batch):
        return self.conv1(dag_batch.x, dag_batch.edge_index)
    

    def _compute_dag_level_embeddings(self, x, dag_batch):
        x_combined = torch.cat([dag_batch.x, x], dim=1)
        y = gnn.global_add_pool(x_combined, dag_batch.batch)
        y = self.mlp_dag(y)
        return y
    

    def _compute_global_embeddings(self, y, env_indptr):
        z = segment_add_csr(y, env_indptr)
        z = self.mlp_global(z)
        return z
        
        
        
class PolicyNetwork(nn.Module):
    def __init__(self, num_node_features, num_dag_features, dim_embed):
        super().__init__()
        self.num_dag_features = num_dag_features
        self.mlp_op_score = make_mlp(num_node_features + 3*dim_embed, 1)
        self.mlp_prlvl_score = make_mlp(num_dag_features + 2*dim_embed + 1, 1)
        
    
    def forward(self,   
        node_x, # node features
        x,      # node embeddings
        y,      # dag embeddings
        z,      # global embeddings
        n_workers,
        num_ops_per_job,
        num_ops_per_env,
        num_jobs_per_env,
        batch_ptr
    ):
        op_scores = self._compute_op_scores(
            node_x, 
            x, 
            y, 
            z, 
            num_ops_per_job, 
            num_ops_per_env)

        # extract dag-level features from
        # node feature tensor
        dag_x = node_x[batch_ptr, :self.num_dag_features]
        
        prlvl_scores = self._compute_prlvl_scores(
            dag_x, 
            y, 
            z, 
            n_workers, 
            num_jobs_per_env)

        return op_scores, prlvl_scores
    
    
    def _compute_op_scores(self, 
        node_x, 
        x, 
        y, 
        z,      
        num_ops_per_job, 
        num_ops_per_env
    ):
        y_repeat = torch.repeat_interleave(y, num_ops_per_job, dim=0)
        
        z_repeat = torch.repeat_interleave(z, num_ops_per_env, dim=0)
        
        op_scores = torch.cat([node_x, x, y_repeat, z_repeat], dim=1)

        op_scores = self.mlp_op_score(op_scores).squeeze(-1)

        return op_scores
    
    
    def _compute_prlvl_scores(self, 
        dag_x, 
        y, 
        z, 
        n_workers, 
        num_jobs_per_env
    ):
        num_total_jobs = num_jobs_per_env.sum()

        limits = torch.arange(n_workers, device=device)
        limits = limits.repeat(num_total_jobs).unsqueeze(1)
        
        y_repeat = torch.repeat_interleave(y, n_workers, dim=0)
        z_repeat = torch.repeat_interleave(z, num_jobs_per_env * n_workers, dim=0)
        
        prlvl_scores = torch.cat([dag_x, limits, y_repeat, z_repeat], dim=1)
        prlvl_scores = prlvl_scores.reshape(num_total_jobs, n_workers, prlvl_scores.shape[1])
        
        prlvl_scores = self.mlp_prlvl_score(prlvl_scores).squeeze(-1)

        return prlvl_scores

    
    
class ActorNetwork(nn.Module):
    def __init__(self, num_node_features, num_dag_features, dim_embed=8):
        super().__init__()

        self.encoder = GraphEncoderNetwork(num_node_features, dim_embed)

        self.policy_network = PolicyNetwork(
            num_node_features, 
            num_dag_features, 
            dim_embed
        )
        
        
    def forward(self, dag_batch, num_jobs_per_env, n_workers):
        env_indptr, num_ops_per_job, num_ops_per_env = \
            self._bookkeep(num_jobs_per_env, dag_batch)

        x, y, z = self.encoder(dag_batch, env_indptr)

        op_scores, prlvl_scores = self.policy_network(
            dag_batch.x,
            x, 
            y, 
            z,
            num_ops_per_job,
            num_ops_per_env,
            num_jobs_per_env,
            n_workers,
            dag_batch.ptr
        )
        
        return op_scores, prlvl_scores, num_ops_per_env, env_indptr


    def _bookkeep(self, num_jobs_per_env, dag_batch):
        num_envs = len(num_jobs_per_env)
        env_indptr = torch.zeros(num_envs+1, device=device, dtype=torch.long)
        torch.cumsum(num_jobs_per_env, 0, out=env_indptr[1:])

        ptr = dag_batch.batch.bincount().cumsum(dim=0)
        dag_batch.ptr = torch.cat([torch.tensor([0], device=device), ptr], dim=0)
        
        num_ops_per_job = dag_batch.ptr[1:] - dag_batch.ptr[:-1]
        num_ops_per_env = segment_add_csr(num_ops_per_job, env_indptr)

        return env_indptr, num_ops_per_job, num_ops_per_env
    