use std::process;
use std::error::Error;
use std::env;
use kdtree::KdTree;
use std::fs;
use std::ffi::OsString;
use geo::prelude::*;
use geo::point;
use rayon::prelude::*;
use delaunator::{Point, triangulate};
use  ordered_float::OrderedFloat;

struct Negocio {
    indice: i32,
    punto: [f64;2],
    cve: String,
    codigo_act: String,
    cve_ent: String,
    cve_mun: String,
    cve_loc: String,
    fecha_alta: String,
    acces: Option<f64>,
    ranking: Option<usize>,
    cluster: Option<usize>,
}

#[derive(Clone)]
struct Edge {
    p: usize,
    q: usize,
    distancia: f64,
}

fn run() -> Result<(), Box<dyn Error>> {

    let directorio = get_arg_n(1)?;
    let file_out = get_arg_n(2)?;

    let dimensions = 2;
    let mut indice = 0;

    let mut kdtree = KdTree::new(dimensions);
    let mut negocios: Vec<Negocio> = Vec::new();

    let paths = fs::read_dir(directorio).unwrap();

    for path in paths {
        let camino = path.unwrap().path().into_os_string();
         sembrador(camino, &mut negocios, &mut kdtree, &mut indice)?;
    }
    println!("Arbol construido! {} registros!",indice);

    accesibilidad(&kdtree, &mut negocios)?;
    println!("Accesibilidades calculadas!");

    let clusters = rankeo(&kdtree, &mut negocios)?;
    println!("{} Clusters inicializados!",clusters);

    jardinero(&kdtree, &mut negocios)?;
    println!("Crecimiento de clusters por distancia terminado!");

    let (asignados, sueltos) = splitter(&mut negocios);
    println!("{} puntos asignados!",asignados.len());
    println!("{} puntos por asignar!",sueltos.len());

    let triangulacion = triangulador(&negocios)?;
    println!("Triangulación lista!");

    tejedor(triangulacion, &mut negocios)?;
    println!("Clusters llenos!");

    let (asignados, sueltos) = splitter(&mut negocios);
    println!("{} puntos asignados!",asignados.len());
    println!("{} puntos por asignar!",sueltos.len());

    totems(sueltos, &kdtree, &mut negocios)?;
    println!("Totems resueltos!");

    let (asignados, sueltos) = splitter(&mut negocios);
    println!("{} puntos asignados!",asignados.len());
    println!("{} puntos por asignar!",sueltos.len());

    escriba(file_out, negocios)?;
    println!("CSV final escrito!");

    Ok(())
}

fn jardinero(arbol: &KdTree<f64, i32, [f64;2]>, vector_datos: &mut Vec<Negocio>) -> Result<(), Box<dyn Error>> {

    let mut centros: Vec<usize> = Vec::new(); 

    for (pos, neg) in  vector_datos.iter_mut().enumerate() {
        match neg.ranking {
            Some(1) => {
                centros.push(pos);
            }
            _ => continue
        }
    };

    for centro in centros {
        let candidatos: Vec<usize> = arbol.nearest(&vector_datos[centro].punto,2000,&geod)
            .unwrap().iter().filter(|ele| ele.0 <= 500.0)
            .map(|ele| *ele.1 as usize).collect();

        for candidato in candidatos {
            match vector_datos[candidato].cluster {
                Some(_) => continue,
                None => vector_datos[candidato].cluster = vector_datos[centro].cluster,
            }
        };
    };

    Ok(())
}

fn triangulador(vector_datos: &Vec<Negocio>) -> Result<delaunator::Triangulation, Box<dyn Error>> {

    let mut puntos = Vec::new();

    for negocio in vector_datos.iter() {
        puntos.push( Point {x: negocio.punto[1], y: negocio.punto[0]} )
    }

    let triangulation = triangulate(&puntos).expect("No triangulation exists.");

    println!("triangles: {:?}", triangulation.triangles.len());
    println!("halfedges: {:?}", triangulation.halfedges.len());

    Ok(triangulation)

}

fn tejedor(triangulation: delaunator::Triangulation, vector_datos: &mut Vec<Negocio>) -> Result<(), Box<dyn Error>> {

    let mut edges: Vec<Edge> = Vec::new();

    edges.par_extend(
        triangulation.triangles.par_iter().enumerate().filter(|(e,_)|{
            *e > triangulation.halfedges[*e]
        })
        .map(|(e,p)| {
            let next = delaunator::next_halfedge(e);
            let q = triangulation.triangles[next];
            let newdist = geod(&vector_datos[*p].punto, &vector_datos[q].punto);
            Edge {
                p: p.clone(),
                q: q.clone(),
                distancia: newdist,
            }
        })
    );

    loop {
        match buscador(vector_datos, &edges) {
            Some(edge) => {
                match vector_datos[edge.p].cluster {
                    Some(clusta) => vector_datos[edge.q].cluster = Some(clusta),
                    None => vector_datos[edge.p].cluster = vector_datos[edge.q].cluster
                }
            },
            None => break
        }
    }

    Ok(())
}

fn buscador(vector_datos: &Vec<Negocio>, edges: &Vec<Edge>) -> Option<Edge> {

    let minimo = edges.par_iter().filter(|edge| {
        let cond_1 = match vector_datos[edge.p].cluster {
            Some(_) => true,
            None => false
        };
        let cond_2 = match vector_datos[edge.q].cluster {
            Some(_) => true,
            None => false
        };
        cond_1 ^ cond_2
    }).min_by_key(|edge| {
        OrderedFloat(edge.distancia)
    });

    match minimo {
        Some(algo) => Some(algo.clone()),
        None => None
    }
}

fn rankeo(arbol: &KdTree<f64, i32, [f64;2]>, vector_datos: &mut Vec<Negocio>) -> Result<usize, Box<dyn Error>> {

    let mut distemp: Vec<Vec<i32>> = Vec::new();

    distemp.par_extend(
        vector_datos.par_iter().map(|neg| {
            let vecinos: usize = ((neg.acces.unwrap().floor() as usize) * 3) + 50;
            arbol.nearest(&neg.punto,vecinos,&geod).unwrap().iter().map(|ele| *ele.1).collect()
        })
    );

    for (pos,disvec) in distemp.iter().enumerate() {
        let mut ranking = 1;
        for inda in disvec.iter() {
            if vector_datos[pos].acces < vector_datos[*inda as usize].acces {
                ranking = ranking + 1;
            }
            if vector_datos[pos].acces == vector_datos[*inda as usize].acces {
                match vector_datos[*inda as usize].ranking {
                    Some(_) => {
                        ranking = ranking + 1;
                    },
                    _ => continue,
                } 
            }
        }
        vector_datos[pos].ranking = Some(ranking);
    };

    let mut clust_count = 0;

    for neg in  vector_datos.iter_mut() {
        match neg.ranking {
            Some(1) => {
                neg.cluster = Some(clust_count);
                clust_count = clust_count + 1;
            }
            _ => continue
        }
    };

    Ok(clust_count)
}

fn totems(vacios: Vec<i32>, arbol: &KdTree<f64, i32, [f64;2]>, vector_datos: &mut Vec<Negocio>) -> Result<(), Box<dyn Error>> {

    for vacio in vacios {
        let candidatos = arbol.nearest(&vector_datos[vacio as usize].punto,2000,&geod).unwrap();
        
        for candidato in candidatos {
            match vector_datos[*candidato.1 as usize].cluster {
                Some(clusta) => {
                    vector_datos[vacio as usize].cluster = Some(clusta);
                    break
                },
                None => continue,
            };
        };
    };
    
    Ok(())
}

fn accesibilidad(arbol: &KdTree<f64, i32, [f64;2]>, vector_datos: &mut Vec<Negocio>) -> Result<(), Box<dyn Error>> {

    vector_datos.par_iter_mut().for_each(|neg| {
        let distancias = arbol.nearest(&neg.punto,2000,&geod).unwrap();
        neg.acces = Some(distancias.iter().fold(0.0, |acc, x| acc + libm::exp(-16.0 * x.0 / 1000.0)));
    });

    Ok(())

}

fn splitter(vector_datos: &mut Vec<Negocio>) -> (Vec<i32>, Vec<i32>) {

    vector_datos.iter().map(|negocio| negocio.indice)
        .partition(|inda|{
        match vector_datos[*inda as usize].cluster {
            Some(_) => true,
            None => false,
        }
    })

}

fn geod(punto1: &[f64], punto2: &[f64]) -> f64 {

    let p1 = point!(x: punto1[0], y: punto1[1]);
    let p2 = point!(x: punto2[0], y: punto2[1]);

    p1.geodesic_distance(&p2)

}

fn get_arg_n(n: usize) -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(n) {
        None => Err(From::from("Se esperaba un argumento, no se encontró ninguno")),
        Some(file_path) => Ok(file_path),
    }
}

fn sembrador(path: OsString, vector_datos: &mut Vec<Negocio>, arbol: &mut KdTree<f64, i32, [f64;2]>, ind: &mut i32) -> Result<(), Box<dyn Error>> {

    let rdr = csv::ReaderBuilder::new()
                .flexible(true)
                .from_path(path)?;

    let mut iter = rdr.into_records();

    loop {
        let row = match iter.next() {
            Some(rec) => rec,
            None => break,
        };

        let pos = iter.reader().position().clone().record();

        let record = match row {
            Ok(rec) => rec,
            Err(_) => {
                println!("No se pudo leer registro {:?}",pos);
                continue
            }
        };

        let latitude: f64 = match record[38].parse() {
            Ok(lat) => lat,
            Err(_) => {
                println!("registro {:?}, lat erronea: {:?}",pos,&record[38]);
                continue
            }
        };

        let longitude: f64 = match record[39].parse() {
            Ok(lon) => lon,
            Err(_) => {
                println!("registro {:?}, lon erronea: {:?}",pos,&record[39]);
                continue
            }
        };

        let cve = &record[0];
        let codigo_act = &record[3];
        let cve_ent = &record[26];
        let cve_mun = &record[28];
        let cve_loc = &record[30];
        let fecha_alta = &record[40];

        let punto: [f64;2] = [longitude,latitude];

        vector_datos.push(Negocio {
            indice: *ind,
            punto: punto,
            cve: String::from(cve),
            codigo_act: String::from(codigo_act),
            cve_ent: String::from(cve_ent),
            cve_mun: String::from(cve_mun),
            cve_loc: String::from(cve_loc),
            fecha_alta: String::from(fecha_alta),
            acces: None,
            ranking: None,
            cluster: None,
        });

        match arbol.add(punto,*ind) {
            _ => {
                *ind = *ind + 1;
                continue}
        }
    }

    Ok(())
}

fn escriba(path: OsString, vector_datos: Vec<Negocio>) -> Result<(), Box<dyn Error>> {

    let mut wtr = csv::Writer::from_path(path)?;

    wtr.write_record(&["cve",
                   "latitud",
                   "longitud",
                   "actividad_cod",
                   "entidad",
                   "municipio",
                   "localidad",
                   "fecha_alta",
                   "accesibilidad",
                   "ranking",
                   "cluster",
                   ])?;


    for neg in vector_datos {

        wtr.serialize((
            &neg.cve, 
            &neg.punto[1], 
            &neg.punto[0],
            &neg.codigo_act,
            &neg.cve_ent,
            &neg.cve_mun,
            &neg.cve_loc,
            &neg.fecha_alta,
            &neg.acces,
            &neg.ranking,
            &neg.cluster,
        ))?;
    
    }

    wtr.flush()?;

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}